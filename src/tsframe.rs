//! 188/204 byte TS packet framing.
//!
//! TODO(phase 2): normalize 204-byte packets to 188 bytes (drop the 16-byte RS
//! parity tail) and skip leading junk up to the first 0x47 sync byte. This
//! happens on the pipeline input side (SPEC §4).

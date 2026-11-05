//! PAT/PMT/SDT parsing (SPEC §8).
//!
//! TODO(phase 2): parse PSI/SI sections into the in-memory service table and
//! decode ARIB service names with `arib::text`.
//!
//! The `arib` crate (0.3, `siketyan/arib-rs`) is already a dependency; the
//! ignored test at the bottom of this file is the Phase 1 probe that proves its
//! STD-B10 (`arib::ts::table`) and STD-B24 (`arib::text`) APIs are usable with
//! the parsing style unituner needs.

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

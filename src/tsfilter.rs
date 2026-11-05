//! Built-in TSFilter for service streams (SPEC §5).
//!
//! TODO(phase 5): rewrite the PAT to a single program (with MPEG-2 CRC32),
//! pass only the target PMT/ES PIDs plus the fixed PIDs, and drop NULL/TEI
//! packets. One instance per subscriber, downstream of the pipeline.

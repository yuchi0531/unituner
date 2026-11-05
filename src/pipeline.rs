//! Pipeline process spawning (SPEC §4).
//!
//! TODO(phase 3): spawn argv-split commands chained stdin->stdout, each in its
//! own process group (`setsid`), and terminate with SIGTERM, a grace period,
//! then SIGKILL to the whole group.
